document.getElementById('uploadForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const fileInput = document.getElementById('videoFile');
  const formData = new FormData();
  formData.append('file', fileInput.files[0]);

  const response = await fetch('/upload/', {
    method: 'POST',
    body: formData
  });

  const result = await response.json();
  document.getElementById('response').innerText = JSON.stringify(result, null, 2);
});
