import io
from aiogram import Router, F
from aiogram.types import CallbackQuery, BufferedInputFile

from database import get_or_create_master, get_statistics, get_clients, get_client_history
from keyboards import stats_keyboard, back_to_menu

router = Router()


@router.callback_query(F.data == "stats")
async def cb_stats(callback: CallbackQuery):
    master_id = await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    stats = await get_statistics(master_id)

    text = "📊 *Статистика*\n\n"
    text += f"👥 Всего клиентов: *{stats['total_clients']}*\n"
    text += f"📅 Всего процедур: *{stats['total_appointments']}*\n"
    text += f"💰 Общая выручка: *{stats['total_earnings']}₽*\n"
    text += f"📆 Выручка за этот месяц: *{stats['month_earnings']}₽*\n"

    if stats["top_procedures"]:
        text += "\n🏆 *Топ процедуры:*\n"
        medals = ["🥇", "🥈", "🥉"]
        for i, (proc, cnt) in enumerate(stats["top_procedures"]):
            text += f"{medals[i]} {proc} — {cnt} раз\n"

    await callback.message.edit_text(text, reply_markup=stats_keyboard(), parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data == "export_excel")
async def cb_export_excel(callback: CallbackQuery):
    await callback.answer("Генерирую файл...")
    master_id = await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    clients = await get_clients(master_id)

    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Клиенты"

        # Шапка таблицы
        headers = ["Имя", "Телефон", "Заметка", "Последний визит", "Кол-во процедур", "Выручка (₽)"]
        ws.append(headers)
        for cell in ws[1]:
            cell.font = Font(bold=True)
            cell.fill = PatternFill("solid", fgColor="FFD9D9")
            cell.alignment = Alignment(horizontal="center")

        # Данные клиентов
        for client in clients:
            cid, name, phone, notes, last_visit = client
            history = await get_client_history(cid)
            total_price = sum(h[2] for h in history if h[2])
            ws.append([
                name,
                phone,
                notes or "",
                last_visit[:10] if last_visit else "нет визитов",
                len(history),
                total_price,
            ])

        # Ширина колонок
        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 40)

        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)

        file = BufferedInputFile(buf.read(), filename="beauty_clients.xlsx")
        await callback.message.answer_document(file, caption="📥 База клиентов выгружена!")

    except ImportError:
        await callback.message.answer(
            "❌ Для экспорта установи библиотеку:\n`pip install openpyxl`",
            parse_mode="Markdown",
            reply_markup=back_to_menu()
        )
