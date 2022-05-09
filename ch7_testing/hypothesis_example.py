from hypothesis import given
from hypothesis import strategies as st
import string

import pytest

import util
names = st.text(alphabet=string.ascii_letters)

@given(names)
def test_names(names):
    print(names)

words=st.lists(st.text(alphabet=string.ascii_letters, min_size=0), min_size=3, max_size=5)
species_st=st.sampled_from(util.species_list + [''])


@st.composite
def description(draw):
    start = draw(words) 
    species = draw(species_st) 
    end = draw(words)
    sentence = ' '.join(start + [species] + end)
    return (species, sentence)


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