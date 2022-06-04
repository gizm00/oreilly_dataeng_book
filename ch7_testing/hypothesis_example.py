from hypothesis import given
from hypothesis import strategies as st
import string


import util
names = st.text(alphabet=string.ascii_letters)

@given(names)
def test_names(names):
    print(names)

words=st.lists(st.text(min_size=0), min_size=3, max_size=5)
species_st=st.sampled_from(util.species_list + [''])


@st.composite
def description(draw):
    start = draw(words) 
    species = draw(species_st) 
    end = draw(words)
    sentence = ' '.join(start + [species] + end).strip()
    return (species, sentence)
