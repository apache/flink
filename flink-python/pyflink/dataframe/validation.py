################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""
Argument checks shared by the DataFrame API.

A wrong type raises :class:`TypeError` and a wrong value of the right type raises
:class:`ValueError`. ``bool`` is rejected wherever a number is expected, since it is an ``int``
subclass and ``take(True)`` is almost certainly a mistake.
"""

import math
from typing import Any, Optional, Sequence, Union


def _require_int(
    value: Any, name: str, minimum: Optional[int] = None, *, maximum: Optional[int] = None,
    include_minimum: bool = True,
) -> None:
    """Require an integer, optionally constrained by lower and upper bounds."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    _require_range(value, name, minimum, maximum, include_minimum)


def _require_number(
    value: Any, name: str, minimum: Optional[float] = None, *, maximum: Optional[float] = None,
    include_minimum: bool = True,
) -> None:
    """Require a finite number, optionally constrained by lower and upper bounds."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number")
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite, got {value}")
    _require_range(value, name, minimum, maximum, include_minimum)


def _require_range(
    value: Union[int, float], name: str, minimum: Optional[float], maximum: Optional[float],
    include_minimum: bool,
) -> None:
    if minimum is not None:
        if include_minimum and value < minimum:
            raise ValueError(f"{name} must be at least {minimum}, got {value}")
        if not include_minimum and value <= minimum:
            raise ValueError(f"{name} must be greater than {minimum}, got {value}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be at most {maximum}, got {value}")


def _require_non_empty_str(value: Any, name: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    if not value:
        raise ValueError(f"{name} must not be empty")


def _require_choice(value: Any, name: str, choices: Sequence[str]) -> None:
    if value not in choices:
        raise ValueError(f"{name} must be one of {list(choices)}, got {value!r}")
