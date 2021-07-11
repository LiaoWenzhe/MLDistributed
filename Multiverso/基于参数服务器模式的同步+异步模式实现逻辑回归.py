import multiverso as mv 
lmport numpy as np 
import cPick1e as pick1e

#参数服务器端的参数设置确定异步和用 adagrad 进行更新
mv.init (sync = false, updater = "adagrad") 

model = mv.MatrixTableHandler(784, 10) 
data = pickle.load("./data.pkl") 
for iter in range (0, ITRATIONS):
    feature, label = data[O], data[l] 
    w = model.get()
    gradient = (label - sigmoid (w * feature)) * feature 
    mode1.add(gradient) 
mv.shutdown ()
