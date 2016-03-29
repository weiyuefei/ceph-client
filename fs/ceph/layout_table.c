#include <linux/ceph/ceph_debug.h>
#include <linux/slab.h>
#include <linux/gfp.h>
#include "layout_table.h"
#include <linux/ceph/libceph.h>

static DEFINE_SPINLOCK(layout_tree_lock);
static struct rb_root layout_tree = RB_ROOT;

int ceph_compare_layout(struct ceph_file_layout *exist,
			struct ceph_file_layout_legacy *legacy,
			const char *pool_ns, size_t pool_ns_len)
{
	struct ceph_file_layout layout;
	ceph_file_layout_from_legacy(&layout, legacy);
	if (exist->stripe_unit != layout.stripe_unit)
		return exist->stripe_unit - layout.stripe_unit;
	if (exist->stripe_count != layout.stripe_count)
		return exist->stripe_count - layout.stripe_count;
	if (exist->object_size != layout.object_size)
		return exist->object_size - layout.object_size;
	if (exist->pool_id != layout.pool_id)
		return exist->pool_id - layout.pool_id;
	if (exist->pool_ns_len != pool_ns_len)
		return exist->pool_ns_len - pool_ns_len;
	if (pool_ns_len > 0)
		return memcmp(exist->pool_ns, pool_ns, pool_ns_len);
	return 0;
}

struct ceph_file_layout *
ceph_find_or_create_layout(struct ceph_file_layout_legacy *legacy,
			   const char *pool_ns, size_t pool_ns_len)
{
	struct __file_layout_node *node, *exist;
	struct rb_node **p, *parent;
	int ret;

	exist = NULL;
	spin_lock(&layout_tree_lock);
	p = &layout_tree.rb_node;
	while (*p) {
		exist = rb_entry(*p, struct __file_layout_node, node);
		ret = ceph_compare_layout(__node_to_layout(exist),
					  legacy, pool_ns, pool_ns_len);
		if (ret > 0)
			p = &(*p)->rb_left;
		else if (ret < 0)
			p = &(*p)->rb_right;
		else
			break;
		exist = NULL;
	}
	if (exist && !kref_get_unless_zero(&exist->kref)) {
		rb_erase(&exist->node, &layout_tree);
		RB_CLEAR_NODE(&exist->node);
		exist = NULL;
	}
	spin_unlock(&layout_tree_lock);
	if (exist)
		return __node_to_layout(exist);

	node = kmalloc(sizeof(*node) + sizeof(struct ceph_file_layout) +
		       pool_ns_len, GFP_NOFS);
	if (!node)
		return NULL;

	kref_init(&node->kref);
	ceph_file_layout_from_legacy(__node_to_layout(node), legacy);
	__node_to_layout(node)->pool_ns_len = pool_ns_len;
	memcpy(__node_to_layout(node)->pool_ns, pool_ns, pool_ns_len);

retry:
	exist = NULL;
	parent = NULL;
	p = &layout_tree.rb_node;
	spin_lock(&layout_tree_lock);
	while (*p) {
		parent = *p;
		exist = rb_entry(*p, struct __file_layout_node, node);
		ret = ceph_compare_layout(__node_to_layout(exist),
					  legacy, pool_ns, pool_ns_len);
		if (ret > 0)
			p = &(*p)->rb_left;
		else if (ret < 0)
			p = &(*p)->rb_right;
		else
			break;
		exist = NULL;
	}
	ret = 0;
	if (!exist) {
		rb_link_node(&node->node, parent, p);
		rb_insert_color(&node->node, &layout_tree);
	} else if (!kref_get_unless_zero(&exist->kref)) {
		rb_erase(&exist->node, &layout_tree);
		RB_CLEAR_NODE(&exist->node);
		ret = -EAGAIN;
	}
	spin_unlock(&layout_tree_lock);
	if (ret == -EAGAIN)
		goto retry;

	if (exist) {
		kfree(node);
		node = exist;
	}

	return __node_to_layout(node);
}

static void ceph_free_layout_node(struct rcu_head *head)
{
	struct __file_layout_node *node =
		container_of(head, struct __file_layout_node, rcu);
	kfree(node);
}

void ceph_release_layout_node(struct kref *ref)
{
	struct __file_layout_node *node =
		container_of(ref, struct __file_layout_node, kref);

	spin_lock(&layout_tree_lock);
	if (!RB_EMPTY_NODE(&node->node)) {
		rb_erase(&node->node, &layout_tree);
		RB_CLEAR_NODE(&node->node);
	}
	spin_unlock(&layout_tree_lock);

	call_rcu(&node->rcu, ceph_free_layout_node);
}

void ceph_layout_table_cleanup(void)
{
	struct rb_node *p;
	struct __file_layout_node  *node;
	if (RB_EMPTY_ROOT(&layout_tree))
		return;

	pr_err("ceph: detect shared layout leaks\n");
	while ((p = rb_first(&layout_tree))) {
		node = rb_entry(p, struct __file_layout_node, node);
		rb_erase(p, &layout_tree);
		kfree(node);
	}
}
