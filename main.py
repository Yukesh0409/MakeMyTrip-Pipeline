import tkinter as tk
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText
import subprocess
import time
import os

def start_job():
    log_output.insert(tk.END, "Starting job...\n")
    log_output.update_idletasks()

    process1 = subprocess.Popen(["python", "data_scrap.py"])
    process1.wait()
    log_output.insert(tk.END, "Script 1 executed\n")
    log_output.update_idletasks()

    process2 = subprocess.Popen(["python", "insert_data.py"])
    process2.wait()
    log_output.insert(tk.END, "Script 2 executed\n")
    log_output.update_idletasks()

    process3 = subprocess.Popen(["python", "transform_data.py"])
    process3.wait()
    log_output.insert(tk.END, "Script 3 executed\n")
    log_output.update_idletasks()

    nbconvert_command = [
        "C:\\ProgramData\\anaconda3\\python.exe",
        "-m",
        "nbconvert",
        "--execute",
        "--to",
        "notebook",
        "--inplace",
        "D:\\Projects\\MakemyTrip\\main\\data_visualization.ipynb"
    ]
    subprocess.Popen(nbconvert_command).wait()
    log_output.insert(tk.END, "Notebook conversion executed\n")
    log_output.update_idletasks()

    notebook_file = "D:\\Projects\\MakemyTrip\\main\\data_visualization.ipynb"
    vscode_command = f"code --wait {notebook_file}"
    subprocess.Popen(vscode_command, shell=True)
    log_output.insert(tk.END, "Notebook opened in VSCode\n")
    log_output.update_idletasks()


root = tk.Tk()
root.title("Job Runner")

canvas = tk.Canvas(root, width=500, height=500)
canvas.pack()

canvas.create_rectangle(0, 0, 500, 500, fill="#4c8bff", width=0)
canvas.create_rectangle(0, 0, 500, 250, fill="#ffffff", width=0)

style = ttk.Style()
style.theme_use("clam")


start_button = ttk.Button(root, text="Start Job", command=start_job)
start_button.place(relx=0.5, rely=0.5, anchor=tk.CENTER)


log_output = ScrolledText(root, wrap=tk.WORD, width=50, height=10)
log_output.pack(pady=10)

root.mainloop()
