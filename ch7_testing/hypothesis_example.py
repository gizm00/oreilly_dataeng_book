from hypothesis import given, composite
from hypothesis import strategies as st
import string

import pytest

from util import species_list
names = st.text(alphabet=string.ascii_letters)

@given(names)
def test_names(names):
    print(names)

words=st.lists(st.text(alphabet=string.ascii_letters, min_size=1), min_size=3, max_size=5)
species=st.sampled_from(species_list + [''])


@composite
def description(draw):
    start = draw(words) 
    species = draw(species) 
    end = draw(words)
    return (species, ' '.join(start + species + end))

@given(description)
def test_description(desc):
    print(desc)
    species = desc[0]
    desc = desc[1]
    if species != '':
        assert species in desc

# @given(st.lists(st.integers()))
# def test_reversing_twice_gives_same_list(xs):
#     # This will generate lists of arbitrary length (usually between 0 and
#     # 100 elements) whose elements are integers.
#     ys = list(xs)
#     ys.reverse()
#     ys.reverse()
#     assert xs == ys


# @given(st.tuples(st.booleans(), st.text()))
# def test_look_tuples_work_too(t):
#     # A tuple is generated as the one you provided, with the corresponding
#     # types in those positions.
#     assert len(t) == 2
#     assert isinstance(t[0], bool)
#     assert isinstance(t[1], str)