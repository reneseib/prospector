import sys
import os
import numpy as np
import numba
from numba import jit, njit, typeof
from numba.core import types
from numba.typed import Dict, List
import timeit
import fiona
from itertools import chain

nb_list = List.empty_list(item_type=types.float64)
nb_nested_list = List(
    [
        nb_list,
        nb_list,
        nb_list,
        nb_list,
    ]
)


collections_list_dict = Dict.empty(key_type=types.int64, value_type=typeof(nb_list))


TREE_collections = Dict.empty(
    key_type=types.unicode_type, value_type=typeof(collections_list_dict)
)


for i in range(4):
    collections_list_dict[i] = nb_list


TREE_collections["parent"] = collections_list_dict
TREE_collections["child"] = collections_list_dict

TREE_leaves = nb_nested_list

print(TREE_leaves)
print("")
print(TREE_collections)


#
# t0 = timeit.default_timer()
#
# geo_file = "/common/ecap/prospector_data/results/stages/3-filtered_by_intersection_protected_area/baden_wuerttemberg/gpkg/baden_wuerttemberg-3-filtered_by_intersection_protected_area.gpkg"
#
#
# def create_typed_dicts():
#     nested_array = np.array([[]]).astype(np.float64)
#
#     int_arr_dict = Dict.empty(key_type=types.int64, value_type=typeof(nested_array))
#
#     str_arr_dict = Dict.empty(
#         key_type=types.unicode_type,
#         value_type=typeof(int_arr_dict),
#     )
#
#     data_dict = Dict.empty(
#         key_type=types.int64,
#         value_type=typeof(str_arr_dict),
#     )
#     return int_arr_dict, str_arr_dict, data_dict
#
#
# int_arr_dict, str_arr_dict, data_dict = create_typed_dicts()
#
#
# t1 = timeit.default_timer() - t0
#
# print("Dict creation took:", t1)
#
# t2 = timeit.default_timer()
# with fiona.open(geo_file) as geo:
#     geo = dict(geo)
#
#     data_keys = list(geo.keys())
#     geometries = {}
#     for k in data_keys:
#         coords = geo[k]["geometry"]["coordinates"]
#         geometries[k] = coords
#
#     for key in data_keys:
#         geom = geometries[key]
#         geom = list(chain(*geom))
#
#         # Copy the typed dictionaries for each geometry
#         int_arr_dict = int_arr_dict.copy()
#         str_arr_dict = str_arr_dict.copy()
#
#         if type(geom[0]) != list:
#             geom = np.array([np.array(x).astype(np.float64) for x in geom]).astype(
#                 np.float64
#             )
#             int_arr_dict[0] = geom.reshape(len(geom), 2)
#
#         else:
#             geom = [np.array(x).astype(np.float64) for x in geom]
#             for i in range(len(geom)):
#                 g = geom[i]
#                 g = g.reshape(len(g), 2)
#
#                 int_arr_dict[i] = g
#
#         str_arr_dict["geometry"] = int_arr_dict
#
#         data_dict[key] = str_arr_dict
#
#
# t3 = timeit.default_timer() - t2
# t4 = timeit.default_timer()
# print("Assigning values took:", t3)
#
# print("In total:", t4 - t0)
#
# # print(data_dict)
