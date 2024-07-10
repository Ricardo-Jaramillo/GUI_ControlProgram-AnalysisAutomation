from GUI import App
import tkinter as tk

root = tk.Tk()
app = App(root)
root.mainloop()

# pyinstaller --onefile --icon="Data Science - Públicos Objetivos\icono_cogno.ico" "Data Science - Públicos Objetivos\main_gui.py"