hey

# Sicherstellen, dass pyspark importiert ist
from pyspark.sql.functions import col, coalesce, lit

# Die EXAMPLE und NEW_TABLE Aliase
df_example_alias = df_example.alias("example")
df_new_table_alias = df_new_table.alias("new_table")

# Definieren der Join-Bedingungen basierend auf TYPE, INDEX, SERIES, und OPERATION
join_conditions = (
    (coalesce(col("example.TYPE"), lit("undefined")) == coalesce(col("new_table.TYPE"), lit("undefined"))) &
    (coalesce(col("example.INDEX"), lit("undefined")) == coalesce(col("new_table.INDEX"), lit("undefined"))) &
    (coalesce(col("example.SERIES"), lit("undefined")) == coalesce(col("new_table.SERIES"), lit("undefined"))) &
    (coalesce(col("example.OPERATION"), lit("undefined")) == coalesce(col("new_table.OPERATION"), lit("undefined")))
)

# Führe den inner Join durch
df_merged = df_example_alias.join(df_new_table_alias, join_conditions, 'inner')

# Optional: relevante Spalten auswählen, falls benötigt
df_selected = df_merged.select(
    col("example.PDC_ID_DE"),
    col("example.PDC_ID_EN"),
    col("example.PDC_CREATION_DATE"),
    col("example.PDC_REPLACEMENT_DATE"),
    col("example.TYPE"),
    col("example.SERIES"),
    col("example.INDEX"),
    col("example.OPERATION"),
    col("example.APPLICATION"),
    col("new_table.PageID"),
    col("new_table.CatID")
)

# Zeige das Ergebnis
df_selected.show()