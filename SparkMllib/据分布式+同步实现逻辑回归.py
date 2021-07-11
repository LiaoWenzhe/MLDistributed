val TERATIONS = 100
val points = spark.textFile ("./sample.txt").map(parsePoint ). persist() 
var w = Vector.random(D) //随机初始向量
for (i < -1 to ITERATIONS) { 
  val gradient = points.map { p = > p.X * (1 / (1 + exp(-p.y * (w dot p.x))) - 1) * p.y}.reduce((a, b) => a + b)
  w -= step_size * gradient
