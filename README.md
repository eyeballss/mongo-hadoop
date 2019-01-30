MR_code directory : It contains map-reduce java codes. For example, Wordcount code, Sort code, Pagerank code... etc  
Data directory(not in git) : It contains data for map-reduce. For example, if you run Wordcount then you can use a word data file here.  
addKeyValue.bash(not in git) : It changes a file to key-value format. This is actually to put a data file into MongoDB.  

  original file :

   a b c d  
   e f g hi  
   jk lm nop qrs  

  changed file : 

   _id	word  
   1	a b c d  
   2	e f g hi  
   3	jk lm nop qrs  

run_hadoop.bash : Its to compile and run MR automatically. It needs 2 arguments, first is java code name, and second is data path in HDFS.
