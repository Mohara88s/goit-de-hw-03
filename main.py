from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round
from colorama import Fore, Style

# Створюємо сесію Spark
spark = SparkSession.builder.appName("MyGoitSpark").getOrCreate()


# 1. Завантажуємо та читаємо кожен CSV-файл як окремий DataFrame.
user_df = spark.read.csv("data/users.csv", header=True, inferSchema=True)
purchase_df = spark.read.csv("data/purchases.csv", header=True, inferSchema=True)
product_df = spark.read.csv("data/products.csv", header=True, inferSchema=True)

print(Fore.GREEN + f"\nЧитаю 5 перших записів кожного DataFrame" + Style.RESET_ALL)
user_df.show(5)
purchase_df.show(5)
product_df.show(5)


# 2. Очищаємо дані, видаляючи будь-які рядки з пропущеними значеннями.
print(Fore.GREEN + f"\nКількість рядків до очищення" + Style.RESET_ALL)
print(f"user: {user_df.count()}")
print(f"purchase: {purchase_df.count()}")
print(f"product: {product_df.count()}")

user_df = user_df.dropna()
purchase_df = purchase_df.dropna()
product_df = product_df.dropna()

print(Fore.GREEN + f"\nКількість рядків після очищення:" + Style.RESET_ALL)
print(f"user: {user_df.count()}")
print(f"purchase: {purchase_df.count()}")
print(f"product: {product_df.count()}")


# 3. Визначаємо загальну суму покупок за кожною категорією продуктів.
print(Fore.GREEN + f"\nЗагальна сума покупок за кожною категорією продуктів:" + Style.RESET_ALL)

sales_sum_by_cat = (purchase_df 
    .join(product_df, "product_id", "inner")
    .groupBy("category")
    .agg(round(sum(col("price") * col("quantity")), 2)
    .alias("sales_sum_by_cat")) 
)

sales_sum_by_cat.show()


# 4. Визначаємо суму покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно.
print(Fore.GREEN + 
    f"\nСума покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно:" 
    + Style.RESET_ALL
)

sales_sum_by_cat_18_25 = (purchase_df 
    .join(product_df, "product_id", "inner")
    .join(user_df, "user_id", "inner")
    .filter((col("age") >= 18) & (col("age") <= 25))
    .groupBy("category")
    .agg(round(sum(col("price") * col("quantity")), 2)
    .alias("sales_sum_by_cat_18_25"))
)

sales_sum_by_cat_18_25.show()


# 5. Визначаємо частку покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років.
print(Fore.GREEN + 
    f"\nЧастка покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років:"
    + Style.RESET_ALL
)

total_sales_sum_18_25 = sales_sum_by_cat_18_25.agg(
    sum("sales_sum_by_cat_18_25").alias("total")
).collect()[0]["total"]

perc_sales_sum_by_cat_18_25 = (sales_sum_by_cat_18_25
    .withColumn("percentage", round((col("sales_sum_by_cat_18_25") / total_sales_sum_18_25) * 100, 2)
))

perc_sales_sum_by_cat_18_25.show()


# 6. Вибераємо 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років.
print(Fore.GREEN + 
    f"\nТоп 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років:" 
    + Style.RESET_ALL
)

top_3_cat = perc_sales_sum_by_cat_18_25.orderBy(
    col("percentage").desc()
).limit(3)

top_3_cat.show()


# Закриваємо сесію Spark
spark.stop()