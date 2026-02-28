export default function Tabs({ active, onChange }) {
  return (
    <div style={styles.wrap}>
      <button style={active === "home" ? styles.active : styles.btn} onClick={() => onChange("home")}>Home</button>
      <button style={active === "mine" ? styles.active : styles.btn} onClick={() => onChange("mine")}>Mine</button>
      <button style={active === "managers" ? styles.active : styles.btn} onClick={() => onChange("managers")}>Managers</button>
    </div>
  );
}

const styles = {
  wrap: {
    display: "grid",
    gridTemplateColumns: "1fr 1fr 1fr",
    gap: 8,
    marginBottom: 12,
  },
  btn: {
    border: "1px solid #315173",
    background: "#182b3f",
    color: "#d8e8ff",
    borderRadius: 10,
    padding: "8px 6px",
    cursor: "pointer",
  },
  active: {
    border: "1px solid #4e88c4",
    background: "#2e78c7",
    color: "white",
    borderRadius: 10,
    padding: "8px 6px",
    cursor: "pointer",
  },
};
