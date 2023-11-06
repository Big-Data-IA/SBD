
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.5.2/firebase-app.js";
import { getAnalytics } from "https://www.gstatic.com/firebasejs/10.5.2/firebase-analytics.js";
import { getDatabase, ref, set, onValue } from "https://www.gstatic.com/firebasejs/10.5.2/firebase-database.js";

const firebaseConfig = {
    apiKey: "AIzaSyBVnWOmAcnGmaY8kKeMkx9t1B1Y-NgkQKE",
    authDomain: "rafael-1b651.firebaseapp.com",
    projectId: "rafael-1b651",
    storageBucket: "rafael-1b651.appspot.com",
    messagingSenderId: "1053548854508",
    appId: "1:1053548854508:web:7fa0f20b51847b10a239c9",
    databaseURL: "https://rafael-1b651-default-rtdb.europe-west1.firebasedatabase.app",
    measurementId: "G-F4889N3TWQ"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);
const db = getDatabase(app);

const dataRef = ref(db, 'data/1');

onValue(dataRef, (snapshot) => {
    const data = snapshot.val();
    if (data) {
        document.getElementById("data-display").textContent = data.contenido;
    } else {
        document.getElementById("data-display").textContent = "No data available.";
    }
});

// Function to add data to Firebase
function addData() {
    const contenidoTexto = document.getElementById("new-data").value;
    set(dataRef, {
        contenido: contenidoTexto
    }).then(() => {
        console.log("Data written successfully");
    }).catch((error) => {
        console.error("Error writing data:", error);
    });;
}
document.querySelector("#button").addEventListener("click", addData);

