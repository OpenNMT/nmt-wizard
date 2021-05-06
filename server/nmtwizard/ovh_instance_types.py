from nmtwizard.capacity import Capacity

# extracted from https://www.ovhcloud.com/en/public-cloud/prices/#compute - might need some update
ovh_capacity_map = {
    "s1-2": Capacity(0, 1),
    "s1-4": Capacity(0, 1),
    "s1-8": Capacity(0, 2),
    "b2-7": Capacity(0, 2),
    "b2-15": Capacity(0, 4),
    "b2-30": Capacity(0, 8),
    "b2-60": Capacity(0, 16),
    "b2-120": Capacity(0, 32),
    "c2-7": Capacity(0, 2),
    "c2-15": Capacity(0, 4),
    "c2-30": Capacity(0, 8),
    "c2-60": Capacity(0, 16),
    "c2-120": Capacity(0, 32),
    "i1-45": Capacity(0, 8),
    "i1-90": Capacity(0, 16),
    "i1-180": Capacity(0, 32),
    "r2-15": Capacity(0, 2),
    "r2-30": Capacity(0, 2),
    "r2-60": Capacity(0, 4),
    "r2-120": Capacity(0, 8),
    "r2-240": Capacity(0, 16),
    "g2-15": Capacity(1, 4),
    "g2-30": Capacity(1, 8),
    "g3-30": Capacity(1, 8),
    "g3-120": Capacity(3, 32),
    "t1-45": Capacity(1, 8),
    "t1-90": Capacity(2, 16),
    "t1-180": Capacity(4, 32)
}
