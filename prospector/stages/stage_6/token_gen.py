from functools import reduce
import math
import time
import random
import numpy as np


def sum(a, b):
    return a + b


def base36encode(number, alphabet="0123456789abcdefghijklmnopqrstuvwxyz"):
    """Converts an integer to a base36 string."""
    if not isinstance(number, int):
        raise TypeError("number must be an integer")

    base36 = ""
    sign = ""

    if number < 0:
        sign = "-"
        number = -number

    if 0 <= number < len(alphabet):
        return sign + alphabet[number]

    while number != 0:
        number, i = divmod(number, len(alphabet))
        base36 = alphabet[i] + base36

    return sign + base36


def get_token(int_val):
    La = [ord(x) for x in list("d93")]
    La = reduce(sum, La)

    bla = ((La + int("d93", 36)) % 1000) + 1
    blub = math.floor(time.time() / 1000) - int_val

    interim = str((bla * blub * 10) + (round(random.uniform(0, 1) * 8)) + 1)

    interim2 = str("".join(reversed(list(interim))))
    result = int(interim2, 10)
    # result = np.base_repr(result, 36)
    result = base36encode(result)

    print(result)

    return None


get_token(10)

# function run(Lt) {
#   var La = "d93"
#     .split("")
#     .map(function (LR) {
#       return LR.charCodeAt();
#     })
#     .reduce(function (Lu, pD) {
#       return Lu + pD;
#     });
#   return parseInt(
#     (
#       (((La + parseInt("d93", 36)) % 1000) + 1) *
#         (Math.floor(new Date() / 1000) - Lt) *
#         10 +
#       Math.round(Math.random() * 8) +
#       1
#     )
#       .toString()
#       .split("")
#       .reverse()
#       .join(""),
#     10
#   ).toString(36);
# }
#
# console.log(run(5));
