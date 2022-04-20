# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["SimHei"]  # 设置字体
plt.rcParams["axes.unicode_minus"] = False  # 该语句解决图像中的“-”负号的乱码问题


# 归一到每5000op，秒为单位
x_ticks = [1, 2, 4, 8, 16, 32]
y_ticks_new = [544, 272.2, 12.53, 8.87, 5.27, 5.21]
y_real_new = [83.51, 37.79, 12.53, 8.87, 5.27, 5.21]
y_ticks_old = [3.61, 3.49, 3.67, 3.55, 3.49, 3.37]
y_real_old = [280.38, 282.59, 274.87, 275.99, 279.64, 278.22]
y_fake_write = [17.92, 9.43, 3.77, 2.02, 1.30, 1.24]
# y_real_old = [i * 18.59+ 67 * (_i+1 ) \
#     if _i <3 else 0 for _i,i in enumerate(y_ticks_old)]
plt.figure(figsize=(6,4))
plt.plot(x_ticks, y_real_new, color='blue', label='优化后', marker='o')
plt.plot(x_ticks, y_real_old, color='orange', label='优化前', marker='o')
plt.legend(loc='upper right')
plt.xlabel("线程数")
plt.ylabel("5000次提交耗时")
plt.title("（1）可线性化读写比1：9")
plt.savefig('speed-write.jpg', dpi=500)

plt.figure(figsize=(6,4))
plt.plot(x_ticks,y_fake_write, color='green', marker='o')
plt.legend(loc='upper right')
plt.xlabel('线程数')
plt.ylabel('5000次提交耗时')
plt.title('（2）非可线性化读写比 9：1')
plt.savefig('speed-nolinear.jpg',dpi=500)


from encoding import merge_image
merge_image('speed-write.jpg','speed-nolinear.jpg','speed-out.jpg')