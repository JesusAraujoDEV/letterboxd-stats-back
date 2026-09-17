const daysOfWeek = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"];
const monthsOfYear = [
  "Enero",
  "Febrero",
  "Marzo",
  "Abril",
  "Mayo",
  "Junio",
  "Julio",
  "Agosto",
  "Septiembre",
  "Octubre",
  "Noviembre",
  "Diciembre",
];

const getISOWeekNumber = (date) => {
  const utcDate = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const dayNum = utcDate.getUTCDay() || 7;
  utcDate.setUTCDate(utcDate.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(utcDate.getUTCFullYear(), 0, 1));
  const diffDays = Math.floor((utcDate - yearStart) / 86400000) + 1;
  return Math.ceil(diffDays / 7);
};

const parseWatchedDate = (value) => {
  if (!value) return null;
  const dateString = String(value).trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateString)) return null;
  const parsedDate = new Date(`${dateString}T00:00:00Z`);
  if (Number.isNaN(parsedDate.getTime())) return null;

  const dayIndex = (parsedDate.getUTCDay() + 6) % 7;
  const monthIndex = parsedDate.getUTCMonth();
  return {
    dateString,
    year: dateString.substring(0, 4),
    watchedDay: daysOfWeek[dayIndex],
    watchedWeek: getISOWeekNumber(parsedDate),
    watchedMonth: monthsOfYear[monthIndex],
    watchedMonthIndex: monthIndex,
  };
};

module.exports = { daysOfWeek, monthsOfYear, getISOWeekNumber, parseWatchedDate };
