"""Image + video frame iteration and export.

Demonstrates: ``TopicView.iter_images()``, ``Exporter.export_frames()``,
``Exporter.export_video()``.

Run:
    python examples/04_image_video_export.py

What you'll see: walk an image topic frame-by-frame, then export a
sample as PNGs, then encode the same window as MP4. Works on both raw
``sensor_msgs/Image`` and JPEG-compressed ``CompressedImage`` topics.
"""

from __future__ import annotations

from _common import ensure_output_dir, ensure_sample_bag, header, section

from resurrector import BagFrame
from resurrector.core.export import Exporter


def main() -> None:
    header("04 — Image / video frame iteration and export")
    bag_path = ensure_sample_bag()
    out = ensure_output_dir()
    bf = BagFrame(bag_path)

    section("Iterate frames from /camera/rgb")
    rgb = bf["/camera/rgb"]
    print(f"  is_image_topic: {rgb.is_image_topic}")
    print(f"  message_count:  {rgb.message_count}")
    for i, (ts_ns, frame) in enumerate(rgb.iter_images()):
        print(f"    frame {i}: t={ts_ns/1e9:.3f}s, shape={frame.shape}, dtype={frame.dtype}")
        if i >= 2:
            print(f"    ... ({rgb.message_count - 3} more)")
            break

    section("Iterate JPEG-compressed frames from /camera/compressed")
    compressed = bf["/camera/compressed"]
    print(f"  message_count: {compressed.message_count}")
    for i, (ts_ns, frame) in enumerate(compressed.iter_images()):
        # iter_images decodes the JPEG bytes to a numpy array transparently.
        print(f"    frame {i}: t={ts_ns/1e9:.3f}s, decoded shape={frame.shape}")
        if i >= 1:
            break

    section("Export first 5 frames as PNGs")
    pngs_dir = out / "rgb_frames"
    pngs_dir.parent.mkdir(parents=True, exist_ok=True)
    exporter = Exporter()
    exporter.export_frames(rgb, output_dir=str(out), max_frames=5)
    saved = sorted((out / "camera_rgb").glob("*.png"))
    print(f"  Wrote {len(saved)} PNG file(s):")
    for p in saved:
        print(f"    {p.name}  ({p.stat().st_size // 1024} KB)")

    section("Export same topic as an MP4 video")
    video_path = out / "rgb_clip.mp4"
    try:
        exporter.export_video(rgb, output_path=video_path, fps=15)
        if video_path.exists():
            print(f"  Wrote {video_path.name}  ({video_path.stat().st_size // 1024} KB)")
            print(f"  Open with: any video player (VLC, QuickTime, etc.)")
    except ImportError as e:
        print(f"  [SKIP] MP4 export needs OpenCV: pip install 'rosbag-resurrector[vision-lite]'")
        print(f"         ({e})")

    print(
        "\n  ✓ iter_images() yields decoded numpy arrays for both raw and\n"
        "    JPEG-compressed image topics. PNG / MP4 export reuse the\n"
        "    same iterator under the hood.\n"
    )


if __name__ == "__main__":
    main()
