const calculateLongestStreak = (logs) => {
  const dates = [
    ...new Set(
      (logs || [])
        .map((log) => log.watchedDate || log.Date)
        .filter((date) => date != null && date !== "")
        .map((date) => String(date).trim()),
    ),
  ].sort();

  if (dates.length === 0) return 0;

  let longestStreak = 1;
  let currentStreak = 1;

  for (let i = 1; i < dates.length; i += 1) {
    const diffDays = Math.ceil(
      Math.abs(new Date(dates[i]) - new Date(dates[i - 1])) / (1000 * 60 * 60 * 24),
    );
    if (diffDays === 1) {
      currentStreak += 1;
      if (currentStreak > longestStreak) longestStreak = currentStreak;
    } else if (diffDays > 1) {
      currentStreak = 1;
    }
  }

  return longestStreak;
};

module.exports = { calculateLongestStreak };
