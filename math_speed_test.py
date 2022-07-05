import timeit
from numba import prange, njit
import numpy as np
import random
import os
from math import sqrt


@njit
def go(pool, nums):
    for i in prange(pool):
        n = sqrt(nums[i])


prep1 = timeit.default_timer()
pool = int(2000000000 * 2.75)
###############################################################################
# OBSERVATIONS:
# ~120mn float numbers can be stored per GB RAM
# It seems performance is best when the numbers are only in RAM as they
# can be read faster than from swapfiles on disk.
#
# Best performances are for '+', '-', '*', '<' and '>'. Divisions '/' are
# approx. 20 times more expensive.
# At the maximum numbers in RAM (c.5.5b numbers in test environment), a speed
# of 0.031 nanoseconds per calculation can be reached. Divisons range around
# 0.66 nanoseconds per calculation at maximum performance.
#
# To square numbers takes approx. 0.04 nanoseconds, whilst sqrt also takes
# approx. 0.03 nanoseconds as the simple calculations.
###############################################################################

nums = np.random.random_sample((pool + 1,))
# nums = np.random.randint(10000, size=pool)
prep2 = timeit.default_timer() - prep1

print(f"Randoms loaded in {prep2} sec")


t1 = timeit.default_timer()

go(pool, nums)
t2 = timeit.default_timer() - t1
print("TOTAL: ", t2, " seconds")
print("PER CALC: ", (t2 / pool * 1000000000), " ns")
