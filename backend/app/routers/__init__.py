"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, versioning

# 固定路径的版本管理路由先注册，避免被资源路由的 /api/{module} 动态路径抢先匹配。
ROUTERS = [versioning, resource]
