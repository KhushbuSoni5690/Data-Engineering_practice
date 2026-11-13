1. I learned to use Python logging for progress messages and argparse for command-line file paths.
2. I practised SQL window functions and learned why Parquet is useful for analytical data.
3. I learned that commits save project history locally, while pushing uploads commits to GitHub.
4. The hardest parts were SQL window functions and understanding Git branches, merges and pull requests.
5. I want to understand SQL window functions better so I can write queries independently.
I learned how to start a Spark session and organise a pipeline into separate functions.
I understood the purpose of pytest by creating small DataFrames and checking that my cleaning functions produced the expected results.
I was confused about pytest and command-line arguments, but I now understand how they fit into building and running a pipeline.
Running Spark in Ubuntu helped me save Parquet files after the write operation failed in Windows.
I want to understand pytest code better and practise more Spark functions.
the built in function was faster because it works directly in its engine and can optimise its instructions. my python function took longer becaise spark sends values to a sperate python worker to run the function, then receive the results,, that takes  extrra time and spark cannot optimise python code inside my function same way.

What clicked for me: A clear pipeline needs tests and checks at key steps so I can confirm it is working as expected.
What was still confusing: I now feel clear about the testing process and how the scripts work.
What my tests or SQL results helped me notice: The tests showed that clean_orders and transform_orders behaved as expected for the small DataFrames and cases we tested. The SQL results helped me understand spending patterns and sales trends.
What I want to practise next: I want to move on to file formats and partitioning, following the next stage of the course.