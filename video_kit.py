
import logging
import asyncio
import helper
import config
import cv2
import numpy as np
import ntpath
import glob
import os
from moviepy.editor import VideoFileClip

backSub = cv2.createBackgroundSubtractorMOG2(history=500, varThreshold=25, detectShadows=False)

def is_background_changed(background_model, frame, threshold=20000):
    fg_mask = background_model.apply(frame)
    _, thresh = cv2.threshold(fg_mask, 250, 255, cv2.THRESH_BINARY)
    non_zero_count = np.count_nonzero(thresh)
    return non_zero_count > threshold

def format_time(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    f = int(100*(seconds % 1))
    seconds = int(seconds % 60)
    return f"{hours:02}_{minutes:02}_{seconds:02}.{f:02}"

def save_frame(frame, time, out_dir):    
    current_time_str = format_time(time)
    file_name = os.path.join(out_dir, f"{current_time_str}{config.IMAGE_EXT}")
    cv2.imwrite(file_name, frame, [int(cv2.IMWRITE_JPEG_QUALITY), 30])

def extract_frames(video_path, out_dir):

    cap = cv2.VideoCapture(video_path)

    ret, prev_frame = cap.read()
    current_time = 0
    last_frame = None
    last_frame_time = None
    first = True
    n=1
    last_saved_time = -2

    while True:
        ret, frame = cap.read()

        if not ret:
            break

        frame_time = cap.get(cv2.CAP_PROP_POS_MSEC) / 1000   
        
        if is_background_changed(backSub, frame):
            if (frame_time - last_saved_time) >= n:
                if last_frame is not None:
                    save_frame(last_frame, last_frame_time, out_dir)
                    last_frame = None
                 
                save_frame(frame, frame_time, out_dir)
                last_saved_time = frame_time
            else:
                last_frame = frame
                last_frame_time = frame_time
            
            current_time = frame_time
        
        prev_frame = frame


    if last_frame is not None:
        save_frame(last_frame, last_frame_time, out_dir)

    cap.release()


    

if __name__ == "__main__":
    for file in glob.glob(f"{config.VIDEO_PATH}/*{config.VIDEO_EXT}"):
        file_name_main = ntpath.basename(file).replace(config.VIDEO_EXT,"").replace("#","-")
        folder = os.path.join(config.IMAGES_PATH, file_name_main)
        print(f"Processing {file_name_main}...")
        if not os.path.exists(folder):
            os.makedirs(folder)

        audio_path = os.path.join(folder, f"{file_name_main}{config.AUDIO_EXT}")
        video_clip = VideoFileClip(file)
        audio_clip = video_clip.audio
        audio_clip.write_audiofile(audio_path)
        audio_clip.close()
        video_clip.close()

        extract_frames(file, folder)