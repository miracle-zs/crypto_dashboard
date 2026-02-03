# 加密货币交易仪表盘 (Crypto Trading Dashboard)

这是一个基于 FastAPI 和币安 API 构建的交易分析与实时监控仪表盘。

## ✨ 主要功能

- **交易分析仪表盘**:
  - 自动从币安 API 同步合约交易历史。
  - 计算并展示关键绩效指标（KPIs），如：总盈亏、胜率、盈亏因子、最大回撤、SQN 等。
  - 以图表形式展示权益曲线。
  - 以表格形式列出所有已完成的交易。
- **资产实时监控**:
  - 提供独立的监控页面，用于实时追踪账户总余额。
  - 每分钟自动从 API 更新余额，并绘制实时权益曲线。
  - 计算并展示风险指标，如当前回撤、波动率和账户健康分。
- **后台任务**:
  - 使用 APScheduler 在后台自动同步交易和余额数据。
  - 数据持久化存储在本地 SQLite 数据库中。

## 🛠️ 技术栈

- **后端**: FastAPI, Python 3
- **数据处理**: Pandas, NumPy
- **API 通信**: Requests
- **定时任务**: APScheduler
- **数据库**: SQLite
- **前端**: HTML, Tailwind CSS, ApexCharts.js
- **开发服务器**: Uvicorn
- **生产部署推荐**: Gunicorn + Nginx

## 🚀 本地开发与运行

### 1. 克隆项目

```bash
git clone <您的仓库URL>
cd crypto_dashboard
```

### 2. 创建并激活虚拟环境

为了隔离项目依赖，建议使用虚拟环境。

```bash
# 创建虚拟环境
python3 -m venv venv

# 激活虚拟环境
# Windows:
# venv\Scripts\activate
# macOS / Linux:
source venv/bin/activate
```

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

### 4. 配置 API 密钥

在项目根目录下创建一个名为 `.env` 的文件。

```bash
# 创建 .env 文件
touch .env
```

然后编辑该文件，填入您的币安 API 密钥和 Secret。**请确保 API 密钥有查询合约交易的权限。**

```env
BINANCE_API_KEY=YOUR_API_KEY
BINANCE_API_SECRET=YOUR_API_SECRET

# (可选) 自定义首次同步的时间范围
# 如果不设置，默认为最近30天
# START_DATE=2023-01-01
# END_DATE=2023-12-31
```

### 5. 启动开发服务器

完成以上步骤后，运行以下命令即可启动应用：

```bash
uvicorn app.main:app --reload
```

服务器启动后，后台会自动开始首次数据同步，这可能需要几分钟时间，具体取决于您的交易历史数量。

## 🌐 访问应用

当您在**本地开发环境**中成功启动服务后：

- **主仪表盘 (交易分析)**:
  - 访问 `http://127.0.0.1:8000`
- **实时监控页**:
  - 访问 `http://127.0.0.1:8000/live-monitor`

您可以在页面头部的导航栏在这两个页面之间轻松切换。

## ☁️ 生产部署

对于生产环境部署，推荐使用 **Gunicorn** 作为应用服务器，并配置 **Nginx** 作为反向代理。详细的部署指南请参考我之前提供的步骤。在生产环境中，Nginx 通常监听 `80` 端口 (HTTP) 或 `443` 端口 (HTTPS)，用户直接通过服务器 IP 或域名访问，无需指定端口号。
