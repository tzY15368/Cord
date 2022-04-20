import matplotlib.pyplot as plt
plt.rcParams["font.sans-serif"] = ["SimHei"]  # 设置字体
plt.rcParams["axes.unicode_minus"] = False  # 该语句解决图像中的“-”负号的乱码问题

# 归一到每5000op，秒为单位
x_ticks = [1, 2, 4, 8, 16, 32]
y_ticks_new = [544, 272.2, 12.53, 8.87, 7.27, 7.21]
y_real_new = [83.51, 37.79, 12.53, 8.87, 7.27, 7.21]
y_ticks_old = [3.61, 3.49, 3.67, 3.55, 3.49, 3.37]
y_real_old = [127.38, 82.59, 74.87, 75.99, 72.64, 78.22]
# y_real_old = [i * 18.59+ 67 * (_i+1 ) \
#     if _i <3 else 0 for _i,i in enumerate(y_ticks_old)]

plt.plot(x_ticks, y_real_new, color='green', label='new')
plt.plot(x_ticks, y_real_old, color='red', label='old')
plt.savefig('speed.jpg',dpi=500)
plt.show()
