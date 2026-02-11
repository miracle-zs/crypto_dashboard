# 加密货币交易仪表盘 (Crypto Trading Dashboard)

这是一个基于 FastAPI 和币安 API 构建的专业级交易分析与实时监控系统。不仅提供机构级的绩效分析，还专为移动端优化，让您随时随地掌控交易状态。

## ✨ 核心功能

### 📊 交易分析 (Analytics)
- **多维度绩效评估**: 自动计算夏普比率 (Sharpe)、卡玛比率 (Calmar)、SQN、凯利公式建议仓位等专业指标。
- **可视化报表**: 交互式权益曲线图、盈亏分布散点图、小时级盈亏热力图。
- **完整交易记录**: 自动同步合约历史订单，计算准确的净盈亏与费率。

### 📱 实时监控 (Live Monitor)
- **驾驶舱模式**: 实时追踪账户总权益、未结盈亏、风险敞口和杠杆率。
- **移动端优先**: 专为手机设计的**卡片式视图**，自动适配屏幕，关键信息一目了然。
- **极速体验**: 采用并行数据加载与骨架屏技术，秒级刷新，拒绝等待。
- **风险风控**: 实时计算当前回撤，配合账户健康度评分系统，及时预警。

### 🛡️ 系统运维
- **实时日志 (Logs)**: 内置 Web 控制台，实时查看系统后台运行状态与 API 交互日志。
- **指标文档 (Metrics)**: 集成完整的量化指标说明书与计算公式，方便查阅。
- **后台自动同步**: 基于 APScheduler 的定时任务，支持断点续传与增量更新。

## 🛠️ 技术栈

- **后端**: FastAPI (Python 3.11+), APScheduler
- **数据存储**: SQLite (本地轻量化存储)
- **数据处理**: Pandas, NumPy
- **前端**: HTML5, Tailwind CSS (响应式), ApexCharts.js (可视化)
- **部署**: Uvicorn / Gunicorn + Nginx

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

# (可选) 同步配置
# 交易同步间隔(分钟)，默认10
# UPDATE_INTERVAL_MINUTES=10
# 同步天数，默认30
# DAYS_TO_FETCH=30

# (可选) Binance 全局请求限速(秒)，默认0.3
# BINANCE_MIN_REQUEST_INTERVAL=0.3
```

> 时间说明：交易同步时间窗口与月度统计按北京时间(UTC+8)计算。

### 5. (可选) 网络代理配置

如果在中国大陆地区运行，通常需要配置代理才能连接币安 API。请在运行前设置环境变量：

**Linux / macOS:**
```bash
export http_proxy=http://127.0.0.1:7890
export https_proxy=http://127.0.0.1:7890
```

**Windows (PowerShell):**
```powershell
$env:http_proxy="http://127.0.0.1:7890"
$env:https_proxy="http://127.0.0.1:7890"
```

### 6. 启动开发服务器

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
