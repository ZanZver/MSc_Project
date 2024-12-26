
def test_connection_logic(w3):
    if not w3 or not w3.is_connected():
        raise HTTPException(status_code=500, detail="Blockchain connection is not active.")
    return {"message": "Connected to blockchain"}