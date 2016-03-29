#ifndef _FS_CEPH_LAYOUT_TABLE_H
#define _FS_CEPH_LAYOUT_TABLE_H

#include <linux/types.h>
#include <linux/kref.h>
#include <linux/rbtree.h>
#include <linux/rcupdate.h>

struct ceph_file_layout;
struct ceph_file_layout_legacy;

struct __file_layout_node {
	struct kref kref;
	union {
		struct rb_node node;
		struct rcu_head rcu;
	};
	u64 __layout[0];
};

static inline struct __file_layout_node *
__layout_to_node(struct ceph_file_layout *layout)
{
	return container_of((u64*)layout, struct __file_layout_node,
			    __layout[0]);
}

static inline struct ceph_file_layout *
__node_to_layout(struct __file_layout_node *node)
{
	return (struct ceph_file_layout *)node->__layout;
}

extern struct ceph_file_layout *
ceph_find_or_create_layout(struct ceph_file_layout_legacy *legacy,
			   const char *pool_ns, size_t pool_ns_len);
extern int ceph_compare_layout(struct ceph_file_layout *exist,
			       struct ceph_file_layout_legacy *legacy,
			       const char *pool_ns, size_t pool_ns_len);
extern void ceph_layout_table_cleanup(void);


static inline void ceph_get_layout(struct ceph_file_layout *layout)
{
	kref_get(&__layout_to_node(layout)->kref);
}

extern void ceph_release_layout_node(struct kref *ref);
static inline void ceph_put_layout(struct ceph_file_layout *layout)
{
	if (!layout)
		return;
	kref_put(&__layout_to_node(layout)->kref, ceph_release_layout_node);
}

#define ceph_try_get_layout(x)						\
({									\
	struct ceph_file_layout *__fl;					\
	rcu_read_lock();						\
	for (;;) {							\
		__fl = rcu_dereference(x);				\
		if (!__fl ||						\
		    kref_get_unless_zero(&__layout_to_node(__fl)->kref))\
			break;						\
	}								\
	rcu_read_unlock();						\
	(__fl);								\
})
#endif
