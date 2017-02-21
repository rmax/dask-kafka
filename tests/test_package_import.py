import dask_kafka


def test_package_metadata():
    assert dask_kafka.__author__
    assert dask_kafka.__email__
    assert dask_kafka.__version__
