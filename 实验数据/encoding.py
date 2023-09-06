# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt


################
merge = False
##############



plt.rcParams["font.sans-serif"]=["SimSun"] #设置字体
plt.rcParams["axes.unicode_minus"]=False #该语句解决图像中的“-”负号的乱码问题
plt.rcParams["mathtext.fontset"] = 'stix'
_times = [1998,21.23]
_alloc = [33,1]
_names = ['gob','gogo-protobuf']
width = 0.4

x1 = []
x2 = []
for i in range(len(_times)):
    x1.append(i)
    x2.append(i+width)
plt.figure(figsize=(6,6))
fig, ax1 = plt.subplots()
ax2 = ax1.twinx()
b1  = ax1.bar(x1,_times,label='每次操作耗时（ns)',width=width,color="orange")
if merge:
    ax1.set_xlabel('（1）序列化库性能对比')

b2 = ax2.bar(x2,_alloc,label='每次操作内存分配（次）',width=width)

plt.xticks([i + 0.2 for i in x1],_names)
plt.setp(ax1.get_xticklabels(),fontsize=18)
plt.setp(ax1.get_yticklabels(),fontsize=18)
plt.setp(ax2.get_yticklabels(),fontsize=18)
plt.legend(handles=[b1,b2],fontsize=13)
plt.savefig('./gob.jpg',dpi=500)

#============================================

_times = [2420,32828]
_alloc = [2,2]
_names = ['mmap','OS write']

_times = list(reversed(_times))
_names = list(reversed(_names))

plt.figure(figsize=(6,6))
fig, ax1 = plt.subplots()
ax2 = ax1.twinx()
b1  = ax1.bar(x1,_times,label='每次操作耗时（ns)',width=width,color="orange")
if merge:
    ax1.set_xlabel('（2）状态持久化底层依赖性能对比')

b2 = ax2.bar(x2,_alloc,label='每次操作内存分配（次）',width=width)

plt.xticks([i + 0.2 for i in x1],_names)

plt.setp(ax1.get_xticklabels(),fontsize=18)
plt.setp(ax1.get_yticklabels(),fontsize=18)
plt.setp(ax2.get_yticklabels(),fontsize=18)
from matplotlib.ticker import MaxNLocator
#plt.gca().xaxis.set_major_locator(MaxNLocator(integer=True))

plt.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
plt.legend(handles=[b1,b2],fontsize=13)
plt.savefig('./mmap.jpg',dpi=500)
def merge_image(i1:str,i2:str,outp:str):
    from PIL import Image
    img1 = Image.open(i1)

    img2 = Image.open(i2)
    out = Image.new('RGB',(img2.size[0]+img1.size[0],img1.size[1])) 
    out.paste(img1) 
    out.paste(img2,(img1.size[0],0))
    out.save(outp,quality=100)

if __name__ == "__main__":

    if not merge:
        exit(0)


merge_image('gob.jpg','mmap.jpg','encoding-out.jpg')