import type { Plan } from '../../lib/io/plan';
import { populatePlanDerivedFields } from '../../lib/io/plan';

export const hydratePlan = <T extends Partial<Plan>>(plan: T): T => {
    populatePlanDerivedFields(plan as Plan);
    return plan;
};
