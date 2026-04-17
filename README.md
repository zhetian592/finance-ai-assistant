# 财经AI决策辅助工具 (FinanceAIAssistant)

定时爬取财经新闻、资金数据，调用 DeepSeek API 分析，生成 HTML 报告，通过 GitHub Actions 自动运行，通过 GitHub Pages 展示。

## 部署步骤

### 1. 创建 GitHub 私有仓库
- 登录 GitHub，点击 New repository
- 仓库名：`finance-ai-tool`，选择 **Private**
- 创建后，将本仓库所有文件上传

### 2. 配置 Secrets
- 进入仓库 Settings → Secrets and variables → Actions
- 点击 New repository secret
- Name: `DEEPSEEK_API_KEY`
- Secret: 你的 DeepSeek API Key

### 3. 启用 GitHub Pages
- 进入仓库 Settings → Pages
- Branch: `main`，文件夹: `/ (root)`
- 保存，等待1分钟

### 4. 手动触发运行
- 进入 Actions 标签页
- 选择 "Daily Finance Analysis"
- 点击 "Run workflow"

### 5. 查看报告
- 访问 `https://你的用户名.github.io/finance-ai-tool/`

## 定时自动运行
- 默认每天北京时间 9:30 自动运行
- 修改 `.github/workflows/daily.yml` 中的 `cron` 表达式可调整时间

## 本地调试
```bash
pip install -r requirements.txt
export DEEPSEEK_API_KEY=你的key
python main.py
