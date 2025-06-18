from elasticsearch_rdsl import greet

def test_greet():
    assert greet("Alice") == "Hello, Alice!"
