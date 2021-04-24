from nmtwizard.capacity import Capacity

# extracted from https://www.ovhcloud.com/en/public-cloud/prices/#compute - might need some update
ovh_capacity_map = {
    "s1-2": Capacity(0, 1),
    "s1-8": Capacity(0, 2),
    "t1-45": Capacity(1, 8)
}
