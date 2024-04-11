# Microsoft Open Source Code of Conduct

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).

Resources:

- [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/)
- [Microsoft Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
- Contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with questions or concerns


 Tags_schema = StructType([StructField('environment',StringType(),True),
                           StructField('owner',StringType(),True),
                           StructField('platform',StringType(),True),
                           StructField('role',StringType(),True),
                           StructField('testcenter',StringType(),True),
                           StructField('owner',StringType(),True)])

df = df.withColumn("_Tags", from_json(df["Tags"], Tags_schema))

df = df.select(
                 df["_Tags.testcenter"].alias("testcenter"))
display(df)
