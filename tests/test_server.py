

def test_server(server):
    assert server.running
    server.terminate()
    assert not server.running
