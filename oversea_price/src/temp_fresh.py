import os
import time,datetime
import platform
import pyautogui
import random


def refresh_desktop():
    system = platform.system()
    if system == "Windows":
        os.system("explorer.exe ::{2559a1f3-21d7-11d4-bdaf-00c04f60b9f0}")

def move_mouse_randomly():
    screen_width, screen_height = pyautogui.size()
    x = random.randint(0, screen_width)
    y = random.randint(0, screen_height)
    pyautogui.moveTo(x, y, duration=0.5)


def main():
    while True:
        refresh_desktop()
        time.sleep(60)
        move_mouse_randomly()
        time.sleep(1800)

if __name__ == "__main__":
    main()
