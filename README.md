hey

import pandas as pd

# CSV-Dateien laden (ersetze die Dateipfade durch die tatsächlichen Pfade)
df1 = pd.read_csv('technical_documentation_MAM_MAP.csv')
df2 = pd.read_csv('NEW_TABLE.csv', delimiter=';')

# Fülle fehlende Werte mit 'undefined', ähnlich wie im Spark-Code mit 'coalesce'
df1['TYPE'] = df1['TYPE'].fillna('undefined')
df1['OPERATION'] = df1['OPERATION'].fillna('undefined')
df1['APPLICATION'] = df1['APPLICATION'].fillna('undefined')
df1['SERIES'] = df1['SERIES'].fillna('undefined')
df1['INDEX'] = df1['INDEX'].fillna('undefined')

df2['TYPE'] = df2['TYPE'].fillna('undefined')
df2['OPERATION'] = df2['OPERATION'].fillna('undefined')
df2['APPLICATION'] = df2['APPLICATION'].fillna('undefined')
df2['SERIES'] = df2['SERIES'].fillna('undefined')
df2['INDEX'] = df2['INDEX'].fillna('undefined')

# Führe den inneren Join durch basierend auf mehreren Spalten
df_merged = pd.merge(df1, df2, on=['TYPE', 'OPERATION', 'APPLICATION', 'SERIES', 'INDEX'], how='inner')

# Relevante Spalten auswählen (ähnlich wie im Spark-Code)
df_selected = df_merged[['PDC_ID_DE', 'PDC_ID_EN', 'PDC_CREATION_DATE', 'PDC_REPLACEMENT_DATE', 'PageID', 'CatID']]

# Ergebnis anzeigen
print(df_selected.head())

# Optional: Ergebnis in einer CSV-Datei speichern
df_selected.to_csv('merged_selected.csv', index=False)
