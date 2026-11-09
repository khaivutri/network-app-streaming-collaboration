import { API } from "../api-client.js";
import { showToast } from "../components/toast.js";

export function renderLogin(root, { onLoggedIn }) {
  root.replaceChildren();
  const page = document.createElement("div");
  page.className = "app-login";
  const frame = document.createElement("header");
  frame.className = "login-frame";
  frame.textContent = "Hệ thống Cộng tác Nội bộ";
  const panel = document.createElement("section");
  panel.className = "login-view panel";
  const title = document.createElement("h1");
  title.textContent = "Đăng nhập hệ thống cộng tác";
  const form = document.createElement("form");
  form.className = "login-form";
  const username = inputField("Tài khoản", "username", "text");
  const password = inputField("Mật khẩu", "password", "password");
  const toggle = document.createElement("button");
  toggle.type = "button";
  toggle.className = "button-secondary password-toggle";
  toggle.textContent = "Hiện mật khẩu";
  toggle.addEventListener("click", () => { password.input.type = password.input.type === "password" ? "text" : "password"; toggle.textContent = password.input.type === "password" ? "Hiện mật khẩu" : "Ẩn mật khẩu"; });
  const submit = document.createElement("button");
  submit.className = "button-primary";
  submit.textContent = "Đăng nhập";
  const error = document.createElement("p");
  error.className = "inline-error";
  error.hidden = true;
  const passwordWrap = document.createElement("div");
  passwordWrap.className = "password-wrap";
  passwordWrap.append(password.input, toggle);
  password.wrapper.append(passwordWrap);
  form.append(username.wrapper, password.wrapper, submit, error);
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    submit.disabled = true;
    username.input.disabled = true;
    password.input.disabled = true;
    submit.textContent = "Đang xác thực...";
    error.hidden = true;
    try {
      await API.login(username.input.value, password.input.value);
      await onLoggedIn();
    } catch (reason) {
      error.textContent = reason.message;
      error.hidden = false;
      password.input.setAttribute("aria-invalid", "true");
      password.input.focus();
      showToast(reason.message, "error");
    } finally {
      submit.disabled = false;
      username.input.disabled = false;
      password.input.disabled = false;
      submit.textContent = "Đăng nhập";
    }
  });
  panel.append(title, form);
  const footer = document.createElement("footer");
  footer.className = "login-footer";
  footer.textContent = "Kết nối an toàn · Hệ thống Cộng tác Nội bộ";
  page.append(frame, panel, footer);
  root.append(page);
  username.input.focus();
}

function inputField(label, name, type) {
  const wrapper = document.createElement("label");
  wrapper.className = "form-field";
  wrapper.textContent = label;
  const input = document.createElement("input");
  input.name = name;
  input.type = type;
  input.autocomplete = name === "username" ? "username" : "current-password";
  input.required = true;
  wrapper.append(input);
  return { wrapper, input };
}
