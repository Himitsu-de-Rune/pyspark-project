import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from my_module import product_with_categories

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
        .master('local[2]') \
        .appName('pytest-pyspark') \
        .getOrCreate()

def test_product_with_and_without_categories(spark):
    products = spark.createDataFrame([
        (1, 'Laptop'),
        (2, 'Phone'),
        (3, 'Table'),
    ], ['product_id', 'product_name'])

    categories = spark.createDataFrame([
        (10, 'Electronics'),
        (20, 'Furniture'),
    ], ['category_id', 'category_name'])

    product_category = spark.createDataFrame([
        (1, 10),
        (2, 10),
    ], ['product_id', 'category_id'])

    result = product_with_categories(products, categories, product_category)

    expected = [
        Row(product_name='Laptop', category_name='Electronics'),
        Row(product_name='Phone', category_name='Electronics'),
        Row(product_name='Table', category_name=None),
    ]

    assert sorted(result.collect(), key=lambda r: (r.product_name, str(r.category_name))) == \
           sorted(expected, key=lambda r: (r.product_name, str(r.category_name)))