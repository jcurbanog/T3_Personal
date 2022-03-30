import numpy as np; np.random.seed(1)
import matplotlib.pyplot as plt

x = (np.logspace(1,10,base=1.8))
y = np.random.rayleigh(size=(1,len(x)))
y = y[0]
def onpick(event):
    ind = event.ind
    datax,datay = event.artist.get_data()
    datax_,datay_ = [datax[i] for i in ind],[datay[i] for i in ind]
    if len(ind) > 1:              
        msx, msy = event.mouseevent.xdata, event.mouseevent.ydata
        dist = np.sqrt((np.array(datax_)-msx)**2+(np.array(datay_)-msy)**2)
        
        ind = [ind[np.argmin(dist)]]
        x = datax[ind]
        y = datay[ind]
    else:
        x = datax_
        y = datay_
    event.artist.get_figure().gca().plot(x,y,'.',color="red")
    event.artist.get_figure().canvas.draw()
    print(x, y)
    

fig,ax = plt.subplots()
ax.plot(x,y,'.',picker=5)

fig.canvas.mpl_connect("pick_event", onpick)

plt.show()