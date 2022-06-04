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

# @given(st.text())
# def test_email(text):
#     print("text:", text)
#     if '@' in text and '.' in text:
#         user = text.split('@')[0]
#         rest = text.split('@')[1]
#         domain = rest.split('.')[0]
#         assert user != ''
#         assert domain != ''
        
@given(st.emails())
def test_email(email):
    print("email:", email)
    user = email.split('@')[0]
    rest = email.split('@')[1]
    domain = rest.split('.')[0]
    assert user != ''
    assert domain != ''