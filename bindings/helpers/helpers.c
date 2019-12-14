#include "helpers.h"

ssize_t drr_effective_payload_len(const dmu_replay_record_t *drr) {
    switch (drr->drr_type) {
        case DRR_OBJECT:
            return DRR_OBJECT_PAYLOAD_SIZE(&drr->drr_u.drr_object);
        case DRR_WRITE:
            return DRR_WRITE_PAYLOAD_SIZE(&drr->drr_u.drr_write);
        case DRR_SPILL:
            return DRR_SPILL_PAYLOAD_SIZE(&drr->drr_u.drr_spill);
        case DRR_BEGIN:
            /* fallthrough */
        case DRR_FREEOBJECTS:
            /* fallthrough */
        case DRR_FREE:
            /* fallthrough */
        case DRR_END:
            /* fallthrough */
        case DRR_WRITE_BYREF:
            /* fallthrough */
        case DRR_WRITE_EMBEDDED:
            /* fallthrough */
        case DRR_OBJECT_RANGE:
            /* fallthrough */
        case DRR_NUMTYPES:
            /* fallthrough */
            break;
    }
    return drr->drr_payloadlen;
}
