from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def product_with_categories(
    products: DataFrame, 
    categories: DataFrame, 
    product_category: DataFrame
) -> DataFrame:
    """
    Возвращает датафрейм со всеми парами (product_name, category_name) 
    и продуктами без категорий (category_name=None).
    """
    return (
        products.alias('p')
        .join(product_category.alias('pc'), F.col('p.product_id') == F.col('pc.product_id'), 'left')
        .join(categories.alias('c'), F.col('pc.category_id') == F.col('c.category_id'), 'left')
        .select(
            F.col('p.product_name').alias('product_name'),
            F.col('c.category_name').alias('category_name')
        )
    )
