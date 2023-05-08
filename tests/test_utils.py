from src.utils.duckaccess import DuckSession

def test_duck_access():
    """
    """
    with DuckSession() as pato:
        resultado = pato.sql("SELECT 42").fetchall()

    assert type(resultado[0][0]) == int
    assert resultado[0][0] == 42