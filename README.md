# Back Market Data Engineering team
**Download this file:** https://backmarket-data-jobs.s3-eu-west-1.amazonaws.com/data/product_catalog.csv

This technical assessment will be evaluated regarding these following points:
- Python Programming
- Clean code
- Interoperabilityæ
- Scalability
- Versioning
- Automatization

### Data Pipeline Assessment
You can develop & refactor your code (using your versioning tool) following this pipeline:
1. Download and read the file: product_catalog.csv locally
2. Transform the file from CSV to Parquet format locally
3. Separate the valid rows from the invalid ones into two separate files: the business wants only the product with an image but wants to archive the invalids rows 

### **Yeah, well done!**

Now Back Market is growing so fast, what you would do to tackle the massive new CSV files?
Describe the next steps for your code to scale it up.

### **Tips:**
1. Do not reinvent the wheel, take the hypothesis
2. Think about the quality of the code
3. Handle the common errors, what if we start again your code?
4. Take the time you need
5. Enjoy!


### Answer : Back Market grows too fast 
We have different options to scale the pipeline. 

First using a distributed data processing framework such as spark, which would allow us to distribute the processing on multiple thread and instances. 

We could compute the split on the dataframe with only the past x minutes of data and run the pipeline every x minutes.

