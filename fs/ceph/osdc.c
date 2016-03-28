#include <linux/ceph/libceph.h>
#include "super.h"
#include "layout_table.h"

/*
 * calculate the mapping of a file extent onto an object, and fill out the
 * request accordingly.  shorten extent as necessary if it crosses an
 * object boundary.
 *
 * fill osd op in request message.
 */
static int calc_layout(struct ceph_file_layout *layout, u64 off, u64 *plen,
			u64 *objnum, u64 *objoff, u64 *objlen)
{
	u64 orig_len = *plen;
	int r;

	/* object extent? */
	r = ceph_calc_file_object_mapping(layout, off, orig_len, objnum,
					  objoff, objlen);
	if (r < 0)
		return r;
	if (*objlen < orig_len) {
		*plen = *objlen;
		dout(" skipping last %llu, final file extent %llu~%llu\n",
		     orig_len - *plen, off, *plen);
	}

	dout("calc_layout objnum=%llx %llu~%llu\n", *objnum, *objoff, *objlen);

	return 0;
}

/*
 * build new request AND message, calculate layout, and adjust file
 * extent as needed.
 *
 * if the file was recently truncated, we include information about its
 * old and new size so that the object can be updated appropriately.  (we
 * avoid synchronously deleting truncated objects because it's slow.)
 *
 * if @do_sync, include a 'startsync' command so that the osd will flush
 * data quickly.
 */
struct ceph_osd_request *ceph_osdc_new_request(struct ceph_osd_client *osdc,
					       struct ceph_inode_info *ci,
					       struct ceph_snap_context *snapc,
					       u64 off, u64 *plen,
					       unsigned int which, int num_ops,
					       int opcode, int flags,
					       u32 truncate_seq,
					       u64 truncate_size,
					       bool use_mempool)
{
	struct ceph_osd_request *req;
	struct ceph_file_layout *layout = &ci->i_layout;
	u64 objnum = 0;
	u64 objoff = 0;
	u64 objlen = 0;
	int r;

	BUG_ON(opcode != CEPH_OSD_OP_READ && opcode != CEPH_OSD_OP_WRITE &&
	       opcode != CEPH_OSD_OP_ZERO && opcode != CEPH_OSD_OP_TRUNCATE &&
	       opcode != CEPH_OSD_OP_CREATE && opcode != CEPH_OSD_OP_DELETE);

	req = ceph_osdc_alloc_request(osdc, snapc, num_ops, use_mempool,
					GFP_NOFS);
	if (!req) {
		r = -ENOMEM;
		goto out_err;
	}

	req->r_flags = flags;

	/* calculate max write size */
	r = calc_layout(layout, off, plen, &objnum, &objoff, &objlen);
	if (r < 0) {
		ceph_osdc_put_request(req);
		goto out_err;
	}

	if (opcode == CEPH_OSD_OP_CREATE || opcode == CEPH_OSD_OP_DELETE) {
		osd_req_op_init(req, which, opcode, 0);
	} else {
		u32 object_size = layout->object_size;
		u32 object_base = off - objoff;
		if (!(truncate_seq == 1 && truncate_size == -1ULL)) {
			if (truncate_size <= object_base) {
				truncate_size = 0;
			} else {
				truncate_size -= object_base;
				if (truncate_size > object_size)
					truncate_size = object_size;
			}
		}
		osd_req_op_extent_init(req, which, opcode, objoff, objlen,
				       truncate_size, truncate_seq);
	}

	req->r_base_oloc.pool = layout->pool_id;

	snprintf(req->r_base_oid.name, sizeof(req->r_base_oid.name),
		 "%llx.%08llx", ci->i_vino.ino, objnum);
	req->r_base_oid.name_len = strlen(req->r_base_oid.name);

	ceph_put_layout(layout);
	return req;
out_err:
	ceph_put_layout(layout);
	return ERR_PTR(r);
}

/*
 * Read some contiguous pages.  If we cross a stripe boundary, shorten
 * *plen.  Return number of bytes read, or error.
 */
int ceph_osdc_readpages(struct ceph_osd_client *osdc,
			struct ceph_inode_info *ci, u64 off, u64 *plen,
			u32 truncate_seq, u64 truncate_size,
			struct page **pages, int num_pages, int page_align)
{
	struct ceph_osd_request *req;
	int rc = 0;

	dout("readpages on ino %llx.%llx on %llu~%llu\n",
	     ci->i_vino.ino, ci->i_vino.snap, off, *plen);
	req = ceph_osdc_new_request(osdc, ci, NULL, off, plen, 0, 1,
				    CEPH_OSD_OP_READ, CEPH_OSD_FLAG_READ,
				    truncate_seq, truncate_size, false);
	if (IS_ERR(req))
		return PTR_ERR(req);

	/* it may be a short read due to an object boundary */

	osd_req_op_extent_osd_data_pages(req, 0,
				pages, *plen, page_align, false, false);

	dout("readpages  final extent is %llu~%llu (%llu bytes align %d)\n",
	     off, *plen, *plen, page_align);

	ceph_osdc_build_request(req, off, NULL, ci->i_vino.snap, NULL);

	rc = ceph_osdc_start_request(osdc, req, false);
	if (!rc)
		rc = ceph_osdc_wait_request(osdc, req);

	ceph_osdc_put_request(req);
	dout("readpages result %d\n", rc);
	return rc;
}

/*
 * do a synchronous write on N pages
 */
int ceph_osdc_writepages(struct ceph_osd_client *osdc,
			 struct ceph_inode_info *ci,
			 struct ceph_snap_context *snapc,
			 u64 off, u64 len,
			 u32 truncate_seq, u64 truncate_size,
			 struct timespec *mtime,
			 struct page **pages, int num_pages)
{
	struct ceph_osd_request *req;
	int rc = 0;
	int page_align = off & ~PAGE_MASK;

	BUG_ON(ci->i_vino.snap != CEPH_NOSNAP);	/* snapshots aren't writeable */
	req = ceph_osdc_new_request(osdc, ci, snapc, off, &len, 0, 1,
				    CEPH_OSD_OP_WRITE,
				    CEPH_OSD_FLAG_ONDISK | CEPH_OSD_FLAG_WRITE,
				    truncate_seq, truncate_size, true);
	if (IS_ERR(req))
		return PTR_ERR(req);

	/* it may be a short write due to an object boundary */
	osd_req_op_extent_osd_data_pages(req, 0, pages, len, page_align,
				false, false);
	dout("writepages %llu~%llu (%llu bytes)\n", off, len, len);

	ceph_osdc_build_request(req, off, snapc, CEPH_NOSNAP, mtime);

	rc = ceph_osdc_start_request(osdc, req, true);
	if (!rc)
		rc = ceph_osdc_wait_request(osdc, req);

	ceph_osdc_put_request(req);
	if (rc == 0)
		rc = len;
	dout("writepages result %d\n", rc);
	return rc;
}
