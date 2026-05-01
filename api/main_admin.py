# Admin endpoints — подключаются в main.py

from fastapi import Depends, HTTPException
from pydantic import BaseModel

class AdminLoginBody(BaseModel):
    password: str


def init_admin(app, verify_admin_token, create_admin_token, ADMIN_SECRET, _jwt_secret):
    @app.post("/admin/api/login")
    async def admin_login(body: AdminLoginBody):
        if body.password != ADMIN_SECRET:
            raise HTTPException(401, "Неверный пароль")
        return {"token": create_admin_token()}

    @app.get("/api/admin/masters", dependencies=[Depends(verify_admin_token)])
    async def admin_list_masters():
        from database import get_all_masters
        masters = await get_all_masters()
        return {"masters": masters}

    @app.get("/api/admin/master/{master_id}/data", dependencies=[Depends(verify_admin_token)])
    async def admin_master_data(master_id: int):
        from database import get_master_full, get_statistics, get_clients_page
        master = await get_master_full(master_id)
        if not master:
            raise HTTPException(404, "Мастер не найден")
        stats = await get_statistics(master_id)
        clients, total = await get_clients_page(master_id, 0, 10000)
        return {"master": master, "stats": stats, "clients": clients, "total_clients": total}

    @app.get("/admin")
    @app.get("/admin/")
    async def serve_admin():
        from fastapi.responses import FileResponse
        return FileResponse("webapp/admin.html")
