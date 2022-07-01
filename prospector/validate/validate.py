#!/usr/bin/python3
from prospector import prospector


def valid_state(state):
    if state in prospector.Prospector.STATES:
        return True


def valid_crs(crs):
    if (
        isinstance(crs, int)
        and crs >= prospector.Prospector.CRS[0]
        and crs <= prospector.Prospector.CRS[len(prospector.Prospector.CRS) - 1]
    ):
        return True


def valid_range(int_range):
    if isinstance(int_range, tuple) and (len(int_range) == 1 or len(int_range) == 2):
        return True
    else:
        return False
