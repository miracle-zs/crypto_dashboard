def test_open_positions_incremental_param_supported(client):
    r = client.get('/api/open-positions?since_version=1')
    assert r.status_code == 200
    body = r.json()
    assert 'version' in body
    assert 'incremental' in body
