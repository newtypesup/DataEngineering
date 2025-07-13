import React, { useState, useEffect } from "react";
import "./App.css";

const categoryOptions = [
  "게임", "도구", "여행", "소셜 미디어", "음악", "맞춤 설정", "오피스", "사진", "글꼴"
];

const ageOptions = [
  "전체이용가", "12세이용가", "15세이용가", "18세이용가"
];

const iconList = [
  "calendar.png", "music.png", "chat.png", "game.png", "photo.png",
  "heart.png", "gear.png", "message.png", "appstore.png"
];

let bubbleId = 0;

function App() {
  const [ctntName, setCtntName] = useState("");
  const [cateName, setCateName] = useState("");
  const [ageRatings, setAgeRatings] = useState("");
  const [bubbles, setBubbles] = useState([]);

  // 비눗방울 생성 함수
  const createBubble = () => {
    const size = 45 + Math.random() * 140;
    const maxSize = 150;
    const minDuration = 6;
    const maxDuration = 16;
    const duration =
      minDuration + ((maxSize - size) / (maxSize - 30)) * (maxDuration - minDuration);

    return {
      id: bubbleId++,
      x: Math.random() * 100,
      size,
      duration,
      icon: iconList[Math.floor(Math.random() * iconList.length)]
    };
  };

  // 버블 생성 반복 제어
  useEffect(() => {
    let intervalId = null;

    const start = () => {
      if (intervalId) return;
      intervalId = setInterval(() => {
        if (document.visibilityState === "visible") {
          setBubbles((prev) => [...prev.slice(-40), createBubble()]);
        }
      }, 500); // 0.5초 간격
    };

    const stop = () => {
      if (intervalId) {
        clearInterval(intervalId);
        intervalId = null;
      }
    };

    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "visible") start();
      else stop();
    });

    start();
    return () => {
      stop();
      document.removeEventListener("visibilitychange", () => {});
    };
  }, []);

  const handleBubbleEnd = (id) => {
    setBubbles((prev) => prev.filter((b) => b.id !== id));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!ctntName || !cateName || !ageRatings) {
      alert("모든 항목을 입력 및 선택해주세요.");
      return;
    }

    const data = {
      ctnt_name: ctntName,
      cate_name: cateName,
      age_ratings: ageRatings
    };

    try {
      const res = await fetch("/contents", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data)
      });

      const result = await res.json();

      if (res.ok) {
        alert("콘텐츠가 성공적으로 등록되었습니다.");
        setCtntName("");
        setCateName("");
        setAgeRatings("");
      } else {
        alert(result.detail || "등록 실패");
      }
    } catch (err) {
      alert("요청 실패: " + err.message);
    }
  };

  return (
    <div className="background">
      {bubbles.map((bubble) => (
        <div
          key={bubble.id}
          className="bubble"
          onAnimationEnd={() => handleBubbleEnd(bubble.id)}
          style={{
            left: `${bubble.x}%`,
            width: `${bubble.size}px`,
            height: `${bubble.size}px`,
            animationDuration: `${bubble.duration}s`
          }}
        >
          <img src={`/icons/${bubble.icon}`} alt="icon" />
        </div>
      ))}

      <div className="app-container">
        <div className="logo">
          <img src="/icons/newtypesup-symbol.png" alt="Logo" />
          <span>newtypesup</span>
        </div>

        <h2>앱 정보 등록</h2>

        <form onSubmit={handleSubmit}>
          <input
            type="text"
            placeholder="앱 이름"
            value={ctntName}
            onChange={(e) => setCtntName(e.target.value)}
          />
          <select value={cateName} onChange={(e) => setCateName(e.target.value)}>
            <option value="">카테고리를 선택하세요</option>
            {categoryOptions.map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
          <select value={ageRatings} onChange={(e) => setAgeRatings(e.target.value)}>
            <option value="">연령 등급을 선택하세요</option>
            {ageOptions.map((r) => (
              <option key={r} value={r}>{r}</option>
            ))}
          </select>
          <button type="submit">등 록</button>
          <div className="bottom-buttons">
            <button type="button">통 계</button>
            <button type="button">예 측</button>
          </div>
        </form>
      </div>
    </div>
  );
}

export default App;
