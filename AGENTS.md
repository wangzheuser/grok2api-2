<!-- PRODUCTION_DEPLOYMENT_POLICY_START -->
## 生产环境 Docker 部署规则入口

当任务涉及 Docker 镜像构建、生产服务器部署、服务更新、容器重启、
Docker Compose、运行配置同步、部署验证或回滚时，执行前必须先完整阅读：

`docs/deployment/production-docker.md`

不可绕过的核心规则：

1. 生产镜像只在本地工作站构建，目标平台固定为 `linux/amd64`。
2. 镜像通过归档上传到服务器，服务器使用 `docker load` 导入。
3. 服务器使用 `docker compose --env-file .env up -d --no-build` 启动。
4. 服务器不执行 `docker build`、`docker buildx build`、`docker compose build`
   或其他等价的镜像构建操作。
5. 后台登录密码只从本地 `.env` 的 `GROK_APP_APP_KEY` 读取；只检查变量是否存在，
   不回显、记录或写入仓库。
6. 常规更新只替换应用镜像，不覆盖服务器已有 `data/`、数据库、配置和其他运行态数据。
7. 服务器连接参数从本机受控配置按需读取，不写入仓库文档。
8. 上传文件、修改生产配置、替换容器或操作生产数据前，按项目危险操作规范取得明确确认。
<!-- PRODUCTION_DEPLOYMENT_POLICY_END -->

<claude-mem-context>
# Memory Context

# [grok2api-2] recent context, 2026-06-02 4:37pm GMT+8

Legend: 🎯session 🔴bugfix 🟣feature 🔄refactor ✅change 🔵discovery ⚖️decision
Format: ID TIME TYPE TITLE
Fetch details: get_observations([IDs]) | Search: mem-search skill

Stats: 21 obs (5,149t read) | 796,031t work | 99% savings

### Jun 2, 2026
4717 9:50a 🔵 项目部署路径查询
4720 " 🔵 Grok2API项目在US服务器的部署路径与配置
4721 9:51a 🔵 Grok2API FlareSolverr服务未启动导致403/500错误
4739 11:08a 🟣 FlareSolverr service added to grok2api Docker Compose stack
4743 11:09a 🔵 Grok2api deployment verified with FlareSolverr integration issues identified
4776 12:02p ✅ 切换到 chrome142 配置进行功能测试
4777 " 🔵 Chrome 142 配置下图片生成 403 认证失败
4778 12:21p 🔵 用户请求问题分析但缺少上下文
4779 " 🔵 grok2api-2项目的Cloudflare Clearance和Imagine功能架构
4780 12:22p 🔵 生产环境FlareSolverr配置验证与CF clearance状态确认
4781 " 🔵 FlareSolverr代理模式差异：直连vs配置代理的cookie返回行为
4782 12:24p 🔵 FlareSolverr代理模式返回代理缓存页面而非真实Grok响应
4783 " 🔵 生产环境clearance配置确认与FlareSolverr设置验证
4784 1:03p 🔵 主会话继续执行已确认
4785 " ✅ 备份 grok2api Docker 部署配置
4787 " 🟣 配置 WARP 和 Privoxy 代理链路
4788 1:04p 🔵 验证 docker-compose.yml 配置结构
4791 1:05p 🟣 完成 grok2api 容器依赖配置并重启服务
4792 " 🟣 部署并验证 WARP 代理链路
4793 1:06p 🔵 验证 FlareSolverr 通过 Privoxy 成功获取 Cloudflare clearance
4794 1:07p 🔵 API 功能验证：对话成功但图片生成返回 403

Access 796k tokens of past work via get_observations([IDs]) or mem-search skill.
</claude-mem-context>
