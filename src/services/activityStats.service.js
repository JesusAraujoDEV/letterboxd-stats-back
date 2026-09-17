const { daysOfWeek, monthsOfYear, parseWatchedDate } = require("../utils/dateHelpers");

const emptyPeriod = () => ({
  days: daysOfWeek.map((day) => ({ day, count: 0 })),
  weeks: Array.from({ length: 52 }, (_, index) => ({ week: index + 1, count: 0 })),
  months: monthsOfYear.map((month) => ({ month, count: 0 })),
});

const buildActivityByYear = (diaryRows) => {
  const activityByYear = { Total: emptyPeriod() };
  const availableYearsSet = new Set();

  diaryRows.forEach((row) => {
    const watchedMeta = parseWatchedDate(row["Watched Date"] || row.watchedDate || null);
    if (!watchedMeta) return;

    const { year, watchedDay, watchedWeek, watchedMonthIndex } = watchedMeta;
    if (!activityByYear[year]) activityByYear[year] = emptyPeriod();
    availableYearsSet.add(year);

    const dayIndex = daysOfWeek.indexOf(watchedDay);
    if (dayIndex >= 0) {
      activityByYear[year].days[dayIndex].count += 1;
      activityByYear.Total.days[dayIndex].count += 1;
    }

    [activityByYear[year], activityByYear.Total].forEach((bucket) => {
      while (bucket.weeks.length < watchedWeek) {
        bucket.weeks.push({ week: bucket.weeks.length + 1, count: 0 });
      }
    });
    if (watchedWeek >= 1) {
      activityByYear[year].weeks[watchedWeek - 1].count += 1;
      activityByYear.Total.weeks[watchedWeek - 1].count += 1;
    }

    if (Number.isFinite(watchedMonthIndex) && watchedMonthIndex < activityByYear[year].months.length) {
      activityByYear[year].months[watchedMonthIndex].count += 1;
      activityByYear.Total.months[watchedMonthIndex].count += 1;
    }
  });

  return { activityByYear, availableYearsSet };
};

const buildWatchedYearStats = (diaryRows) => {
  const watchedYearMap = {};

  diaryRows.forEach((row) => {
    const dateValue = row["Watched Date"] || row.watchedDate || row.date || null;
    if (!dateValue) return;
    const year = String(dateValue).trim().substring(0, 4);
    if (!/^[0-9]{4}$/.test(year)) return;

    if (!watchedYearMap[year]) watchedYearMap[year] = { year, count: 0, ratingSum: 0, ratingCount: 0 };
    watchedYearMap[year].count += 1;

    const rating = parseFloat(row.Rating);
    if (Number.isFinite(rating)) {
      watchedYearMap[year].ratingSum += rating;
      watchedYearMap[year].ratingCount += 1;
    }
  });

  return Object.values(watchedYearMap)
    .map((entry) => ({
      year: entry.year,
      count: entry.count,
      averageRating: entry.ratingCount > 0 ? Number((entry.ratingSum / entry.ratingCount).toFixed(2)) : 0,
    }))
    .sort((a, b) => a.year.localeCompare(b.year));
};

const buildActivityStats = (diaryRows) => {
  const { activityByYear, availableYearsSet } = buildActivityByYear(diaryRows);
  const availableYears = ["Total", ...Array.from(availableYearsSet).sort((a, b) => b.localeCompare(a))];

  return {
    activityStats: { availableYears, byYear: activityByYear },
    watchedYearStats: buildWatchedYearStats(diaryRows),
  };
};

module.exports = { buildActivityStats };
