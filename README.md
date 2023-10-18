# CQ_compiler
You might run into an error referring to logging. In that case add the following to Run -> Edit Configurations... -> Modify options -> Add VM options:
--add-opens=java.base/java.nio=ALL-UNNAMED
