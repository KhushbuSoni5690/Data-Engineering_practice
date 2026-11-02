A1 :
My pipeline started with 200 shuffle partitions, which are like 
baskets holding data. Spark automatically combined them into 9 
bigger baskets, and I checked this in the Spark UI. My computer
had 8 CPU helpers, so it could work on up to 8 baskets at once 
and handle the remaining one when a helper became free. The 9 
final baskets were saved as 9 Parquet files, which matched the
9 partitions in my logs and 9 completed write tasks in the 
Spark UI.

A2 :
Using coalesce(1) created 1 Parquet file, while repartition(4) 
created 4 files. One huge file can make writing slow because one
task does all the work, while many tiny files waste time because 
Spark must manage and open each file.

A3 :
With the null count, my pipeline took 13.21 seconds and completed
6 jobs. Without it, it took 10.30 seconds and completed 3 jobs. 
I chose to remove the separate count because it triggered extra
work only for logging, while leaving the cleaned output
unchanged.(measured time of 11.80 seconds.)

A4 :
I added an is_amount_missing column to flag missing amounts and 
checked that it was saved in the Parquet output. I kept the 
warning by counting missing amounts during the write using 
Observation, avoiding a separate counting pass.

A5 :
Completed 5 pytest.

A6 :
When I removed dropDuplicates, the duplicate-removal test failed 
with assert 3 == 2. It expected 2 rows, but the duplicate 
remained, giving 3 rows. After restoring the line, all 5 tests 
passed again.

A7 :
I ran each version five times and ignored the first timing. 
The built-in median was 0.235 seconds, while the Python UDF 
median was 1.292 seconds. In this comparison, the Python UDF 
took about 5.5 times as long.

A8 :
the built in function was faster because it works directly in 
its engine and can optimise its instructions. my python function 
took longer becaise spark sends values to a sperate python worker
to run the function, then receive the results,, that takes  extrra time and spark cannot optimise python code inside my 
function same way.

A9 :
 1. there are two parts of the spark work, plan and action, when filter 
 is alone just prepare the plan but count is where a plan will be executed 
 so if we remove the count spark does not have to take any action and it will not 
 go through whole dataset, so its faster.
 2.customer table was small, so spark has decided automatically to send it 
 to executers to join. and it became the broadcast join.