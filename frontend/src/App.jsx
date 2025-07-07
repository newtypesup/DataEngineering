import React, { useState } from "react";

const categoryOptions = [
  "게임", "도구", "여행", "소셜 미디어",
  "음악", "맞춤 설정", "오피스", "사진", "글꼴"
];

const ageOptions = [
  "전체이용가", "12세이용가", "15세이용가", "18세이용가"
];

function App() {
  const [ctntName, setCtntName] = useState("");
  const [cateName, setCateName] = useState("");
  const [ageRatings, setAgeRatings] = useState("");

  const handleSubmit = async (e) => {
    e.preventDefault();  // 새로고침 방지

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
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify(data)
      });

      const result = await res.json();

      if (res.ok) {
        alert("성공적으로 저장되었습니다.");
        console.log("서버 응답:", result);
      } else {
        alert(result.detail || "오류 발생");
      }
    } catch (err) {
      alert("서버 요청 실패: " + err.message);
    }
  };

  return (
    <form onSubmit={handleSubmit} style={{ padding: "2rem" }}>
      <h2>콘텐츠 등록</h2>
      
      <input
        type="text"
        placeholder="컨텐츠 이름"
        value={ctntName}
        onChange={(e) => setCtntName(e.target.value)}
      /><br /><br />

      <select value={cateName} onChange={(e) => setCateName(e.target.value)}>
        <option value="">카테고리를 선택하세요</option>
        {categoryOptions.map((category) => (
          <option key={category} value={category}>{category}</option>
        ))}
      </select><br /><br />

      <select value={ageRatings} onChange={(e) => setAgeRatings(e.target.value)}>
        <option value="">연령 등급을 선택하세요</option>
        {ageOptions.map((rating) => (
          <option key={rating} value={rating}>{rating}</option>
        ))}
      </select><br /><br />

      <button type="submit">제출</button>
    </form>
  );
}

export default App;
