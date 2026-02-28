import { useEffect, useRef } from "react";

export default function CanvasScene() {
  const ref = useRef(null);

  useEffect(() => {
    const canvas = ref.current;
    if (!canvas) return;

    const ctx = canvas.getContext("2d");
    let frame = 0;
    let raf = 0;

    const draw = () => {
      frame += 1;
      const w = canvas.width;
      const h = canvas.height;

      ctx.clearRect(0, 0, w, h);
      ctx.fillStyle = "#0a1b2a";
      ctx.fillRect(0, 0, w, h);

      const minerX = 20 + (frame % 130);
      const elevatorY = 18 + ((frame % 100) / 100) * 80;
      const courierX = 16 + ((frame * 1.5) % (w - 36));

      ctx.fillStyle = "#f2b035";
      ctx.fillRect(minerX, h - 45, 18, 18);

      ctx.fillStyle = "#4fc3f7";
      ctx.fillRect(w / 2 - 7, elevatorY, 14, 28);

      ctx.fillStyle = "#7ed957";
      ctx.fillRect(courierX, h - 18, 22, 10);

      ctx.fillStyle = "#d7ecff";
      ctx.font = "12px sans-serif";
      ctx.fillText("Miner / Elevator / Courier", 10, 16);

      raf = requestAnimationFrame(draw);
    };

    draw();
    return () => cancelAnimationFrame(raf);
  }, []);

  return <canvas ref={ref} width={360} height={150} style={styles.canvas} />;
}

const styles = {
  canvas: {
    width: "100%",
    height: "auto",
    borderRadius: 10,
    border: "1px solid rgba(255,255,255,0.2)",
  },
};
