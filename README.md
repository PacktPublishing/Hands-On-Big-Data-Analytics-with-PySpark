# Hands-On Big Data Analytics with PySpark

<a href="https://prod.packtpub.com/in/big-data-and-business-intelligence/hands-big-data-analytics-pyspark?utm_source=github&utm_medium=repository&utm_campaign=9781838644130"><img src="https://prod.packtpub.com/media/catalog/product/cache/a22c7d190d97ca25f5f1089471ab8502/b/1/b14139.png" alt="Hands-On Big Data Analytics with PySpark" height="256px" align="right"></a>

This is the code repository for [Hands-On Big Data Analytics with PySpark](https://prod.packtpub.com/in/big-data-and-business-intelligence/hands-big-data-analytics-pyspark?utm_source=github&utm_medium=repository&utm_campaign=9781838644130), published by Packt.

**Analyze large datasets and discover techniques for testing, immunizing, and parallelizing Spark jobs**

## What is this book about?
Apache Spark is an open source parallel-processing framework that has been around for quite some time now. One of the many uses of Apache Spark is for data analytics applications across clustered computers. In this book, you will not only learn how to use Spark and the Python API to create high-performance analytics with big data, but also discover techniques for testing, immunizing, and parallelizing Spark jobs.

This book covers the following exciting features:
* Get practical big data experience while working on messy datasets
* Analyze patterns with Spark SQL to improve your business intelligence
* Use PySpark's interactive shell to speed up development time
* Create highly concurrent Spark programs by leveraging immutability
* Discover ways to avoid the most expensive operation in the Spark API: the shuffle operation
* Re-design your jobs to use reduceByKey instead of groupBy

If you feel this book is for you, get your [copy](https://www.amazon.com/dp/183864413X) today!

<a href="https://www.packtpub.com/?utm_source=github&utm_medium=banner&utm_campaign=GitHubBanner"><img src="https://raw.githubusercontent.com/PacktPublishing/GitHub/master/GitHub.png" 
alt="https://www.packtpub.com/" border="5" /></a>


## Instructions and Navigations
All of the code is organized into folders. For example, Chapter02.

The code will look like the following:
```
test("Should use immutable DF API") {
 import spark.sqlContext.implicits._
 //given
 val userData =
 spark.sparkContext.makeRDD(List(
 UserData("a", "1"),
 UserData("b", "2"),
 UserData("d", "200")
 )).toDF()
```

**Following is what you need for this book:**
This book is for developers, data scientists, business analysts, or anyone who needs to reliably analyze large amounts of large-scale, real-world data. Whether you're tasked with creating your company's business intelligence function or creating great data platforms for your machine learning models, or are looking to use code to magnify the impact of your business, this book is for you.

With the following software and hardware list you can run all code files present in the book (Chapter 1-13).

### Software and Hardware List

| Chapter  | Software required                      | OS required                        |
| -------- | ---------------------------------------| -----------------------------------|
| 1-6      | Apache Spark (latest version), PySpark |Windows, Mac OS X, and Linux (Any)  |
| 7-13     | Intellij IDEA, Java, Jupyter Notebook  | Windows, Mac OS X, and Linux (Any) |

We also provide a PDF file that has color images of the screenshots/diagrams used in this book. [Click here to download it](http://www.packtpub.com/sites/default/files/downloads/9781838644130_ColorImages.pdf).

### Related products <Other books you may enjoy>
* Learning PySpark [[Packt]](https://prod.packtpub.com/in/big-data-and-business-intelligence/learning-pyspark?utm_source=github&utm_medium=repository&utm_campaign=9781786463708) [[Amazon]](https://www.amazon.com/dp/1786463709)

* PySpark Cookbook [[Packt]](https://prod.packtpub.com/in/big-data-and-business-intelligence/pyspark-cookbook?utm_source=github&utm_medium=repository&utm_campaign=9781788835367) [[Amazon]](https://www.amazon.com/dp/1788835360)

## Get to Know the Authors
**Colibri Digital**
 is a technology consultancy company founded in 2015 by James Cross and Ingrid Funie. The company works to help its clients navigate the rapidly changing and complex world of emerging technologies, with deep expertise in areas such as big data, data science, machine learning, and cloud computing. Over the past few years, they have worked with some of the world's largest and most prestigious companies, including a tier 1 investment bank, a leading management consultancy group, and one of the world's most popular soft drinks companies, helping each of them to better make sense of their data, and process it in more intelligent ways. The company lives by its motto: Data -> Intelligence -> Action.

**Rudy Lai**
 is the founder of QuantCopy, a sales acceleration start-up using AI to write sales emails to prospective customers. Prior to founding QuantCopy, Rudy ran HighDimension.IO, a machine learning consultancy, where he experienced first hand the frustrations of outbound sales and prospecting. Rudy has also spent more than 5 years in quantitative trading at leading investment banks such as Morgan Stanley. This valuable experience allowed him to witness the power of data, but also the pitfalls of automation using data science and machine learning. He holds a computer science degree from Imperial College London, where he was part of the Dean's list, and received awards including the Deutsche Bank Artificial Intelligence prize.

**Bartłomiej Potaczek**
 is a software engineer working for Schibsted Tech Polska and
programming mostly in JavaScript. He is a big fan of everything related to the react world, functional programming, and data visualization. He founded and created InitLearn, a portal that allows users to learn to program in a pair-programming fashion. He was also involved in InitLearn frontend, which is built on the React-Redux technologies. Besides programming, he enjoys football and crossfit. Currently, he is working on rewriting the frontend for tv.nu—Sweden's most complete TV guide, with over 200 channels. He has also recently worked on technologies including React, React Router, and Redux.


### Suggestions and Feedback
[Click here](https://docs.google.com/forms/d/e/1FAIpQLSdy7dATC6QmEL81FIUuymZ0Wy9vH1jHkvpY57OiMeKGqib_Ow/viewform) if you have any feedback or suggestions.
### Download a free PDF

 <i>If you have already purchased a print or Kindle version of this book, you can get a DRM-free PDF version at no cost.<br>Simply click on the link to claim your free PDF.</i>
<p align="center"> <a href="https://packt.link/free-ebook/9781838644130">https://packt.link/free-ebook/9781838644130 </a> </p>