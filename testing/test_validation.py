from hypothesis import given
from hypothesis import strategies as st

#https://stackoverflow.com/questions/201323/how-can-i-validate-an-email-address-using-a-regular-expression

def is_valid_email(sample):
    try:
        assert '@' in sample 
        assert sample.endswith(".com")
        return True
    except AssertionError:
        return False

@given(st.emails())
def test_is_valid_email(emails):
    print("email:", emails)
    assert is_valid_email(emails)

    
        
